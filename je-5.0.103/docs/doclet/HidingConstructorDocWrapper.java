/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ConstructorDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.ParamTag;
import com.sun.javadoc.Parameter;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;
import com.sun.javadoc.ThrowsTag;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;

class HidingConstructorDocWrapper extends HidingWrapper 
                                  implements ConstructorDoc {
    public HidingConstructorDocWrapper(ConstructorDoc constrdoc, 
                                       Map mapWrappers) {
        super(constrdoc, mapWrappers);
    }

    private ConstructorDoc _getConstructorDoc() {
        return (ConstructorDoc)getWrappedObject();
    }

    public String qualifiedName() {
        return _getConstructorDoc().qualifiedName();
    }

    public TypeVariable[] typeParameters() {
        return (TypeVariable[])
                wrapOrHide(_getConstructorDoc().typeParameters());
    }

    public ParamTag[] typeParamTags() {
        return (ParamTag[])wrapOrHide(_getConstructorDoc().typeParamTags());
    }

    public boolean isVarArgs() {
        return _getConstructorDoc().isVarArgs();
    }

    public Type[] thrownExceptionTypes() {
        return (Type[])
                wrapOrHide(_getConstructorDoc().thrownExceptionTypes());
    }
  
    public String commentText() {
        return _getConstructorDoc().commentText();
    }

    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getConstructorDoc().
                   compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {
            return _getConstructorDoc().compareTo(obj);
        }
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getConstructorDoc().firstSentenceTags());
    }

    public String getRawCommentText() {
        return _getConstructorDoc().getRawCommentText();
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getConstructorDoc().inlineTags());
    }

    public boolean isClass() {
        return _getConstructorDoc().isClass();
    }

    public boolean isConstructor() {
        return _getConstructorDoc().isConstructor();
    }

    public boolean isError() {
        return _getConstructorDoc().isError();
    }

    public boolean isException() {
        return _getConstructorDoc().isException();
    }

    public boolean isField() {
        return _getConstructorDoc().isField();
    }

    public boolean isIncluded() {
        return _getConstructorDoc().isIncluded();
    }

    public boolean isInterface() {
        return _getConstructorDoc().isInterface();
    }

    public boolean isMethod() {
        return _getConstructorDoc().isMethod();
    }

    public boolean isOrdinaryClass() {
        return _getConstructorDoc().isOrdinaryClass();
    }

    public String name() {
        return _getConstructorDoc().name();
    }

    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getConstructorDoc().seeTags());
    }

    public void setRawCommentText(String szText) {
        _getConstructorDoc().setRawCommentText(szText);
    }

    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getConstructorDoc().tags());
    }

    public Tag[] tags(String szTagName) {
        return (Tag[])wrapOrHide(_getConstructorDoc().tags(szTagName));
    }
   
    public SourcePosition position() {
        return _getConstructorDoc().position();
    }

    public boolean isAnnotationType() {
        return _getConstructorDoc().isAnnotationType();
    }

    public boolean isEnum() {
        return _getConstructorDoc().isEnum();
    }

    public boolean isAnnotationTypeElement() {
        return _getConstructorDoc().isAnnotationTypeElement();
    }

    public boolean isEnumConstant() {
        return _getConstructorDoc().isEnumConstant();
    }
  
    public boolean isSynthetic() {
        return _getConstructorDoc().isSynthetic();
    }

    public AnnotationDesc[] annotations() {
        return (AnnotationDesc[])
                wrapOrHide(_getConstructorDoc().annotations());
    }

    public boolean isFinal() {
        return _getConstructorDoc().isFinal();
    }

    public ClassDoc containingClass() {
        return (ClassDoc)wrapOrHide(_getConstructorDoc().containingClass());
    }

    public PackageDoc containingPackage() {
        return (PackageDoc)wrapOrHide(_getConstructorDoc().containingPackage());
    }

    public boolean isPackagePrivate() {
        return _getConstructorDoc().isPackagePrivate();
    }

    public boolean isPrivate() {
        return _getConstructorDoc().isPrivate();
    }

    public boolean isProtected() {
        return _getConstructorDoc().isProtected();
    }

    public boolean isPublic() {
        return _getConstructorDoc().isPublic();
    }

    public boolean isStatic() {
        return _getConstructorDoc().isStatic();
    }

    public int modifierSpecifier() {
        return _getConstructorDoc().modifierSpecifier();
    }

    public String modifiers() {
        return _getConstructorDoc().modifiers();
    }
  
    public String flatSignature() {
        return _getConstructorDoc().flatSignature();
    }

    public boolean isNative() {
        return _getConstructorDoc().isNative();
    }

    public boolean isSynchronized() {
        return _getConstructorDoc().isSynchronized();
    }

    public ParamTag[] paramTags() {
        return (ParamTag[])wrapOrHide(_getConstructorDoc().paramTags());
    }

    public Parameter[] parameters() {
        return (Parameter[])wrapOrHide(_getConstructorDoc().parameters());
    }

    public String signature() {
        return _getConstructorDoc().signature();
    }

    public ClassDoc[] thrownExceptions() {
        return (ClassDoc[])wrapOrHide(_getConstructorDoc().thrownExceptions());
    }

    public ThrowsTag[] throwsTags() {
        return (ThrowsTag[])wrapOrHide(_getConstructorDoc().throwsTags());
    }
}
