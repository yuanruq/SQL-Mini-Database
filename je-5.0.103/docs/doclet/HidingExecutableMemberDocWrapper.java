/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ExecutableMemberDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.ParamTag;
import com.sun.javadoc.Parameter;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;
import com.sun.javadoc.ThrowsTag;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;

class HidingExecutableMemberDocWrapper extends HidingWrapper
                                       implements ExecutableMemberDoc {
    
    public HidingExecutableMemberDocWrapper(ExecutableMemberDoc execmemdoc,
                                            Map mapWrappers) {
        super(execmemdoc, mapWrappers);
    }

    private ExecutableMemberDoc _getExecutableMemberDoc() {
        return (ExecutableMemberDoc)getWrappedObject();
    }

    public String flatSignature() {
        return _getExecutableMemberDoc().flatSignature();
    }

    public boolean isNative() {
        return _getExecutableMemberDoc().isNative();
    }

    public boolean isSynchronized() {
        return _getExecutableMemberDoc().isSynchronized();
    }

    public ParamTag[] paramTags() {
        return (ParamTag[])wrapOrHide(_getExecutableMemberDoc().paramTags());
    }

    public Parameter[] parameters() {
        return (Parameter[])wrapOrHide(_getExecutableMemberDoc().parameters());
    }

    public String signature() {
        return _getExecutableMemberDoc().signature();
    }

    public ClassDoc[] thrownExceptions() {
        return (ClassDoc[])
                wrapOrHide(_getExecutableMemberDoc().thrownExceptions());
    }

    public ThrowsTag[] throwsTags() {
        return (ThrowsTag[])
                wrapOrHide(_getExecutableMemberDoc().throwsTags());
    }

    public TypeVariable[] typeParameters() {
        return (TypeVariable[])
                wrapOrHide(_getExecutableMemberDoc().typeParameters());
    }

    public ParamTag[] typeParamTags() {
        return (ParamTag[])
                wrapOrHide(_getExecutableMemberDoc().typeParamTags());
    }

    public boolean isVarArgs() {
        return _getExecutableMemberDoc().isVarArgs();
    }

    public Type[] thrownExceptionTypes() {
        return (Type[])
                wrapOrHide(_getExecutableMemberDoc().thrownExceptionTypes());
    }

    public String commentText() {
        return _getExecutableMemberDoc().commentText();
    }

    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getExecutableMemberDoc().
                   compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {
            return _getExecutableMemberDoc().compareTo(obj);
        }
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getExecutableMemberDoc().firstSentenceTags());
    }

    public String getRawCommentText() {
        return _getExecutableMemberDoc().getRawCommentText();
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getExecutableMemberDoc().inlineTags());
    }

    public boolean isClass() {
        return _getExecutableMemberDoc().isClass();
    }

    public boolean isConstructor() {
        return _getExecutableMemberDoc().isConstructor();
    }

    public boolean isError() {
        return _getExecutableMemberDoc().isError();
    }

    public boolean isException() {
        return _getExecutableMemberDoc().isException();
    }

    public boolean isField() {
        return _getExecutableMemberDoc().isField();
    }

    public boolean isIncluded() {
        return _getExecutableMemberDoc().isIncluded();
    }

    public boolean isInterface() {
        return _getExecutableMemberDoc().isInterface();
    }

    public boolean isMethod() {
        return _getExecutableMemberDoc().isMethod();
    }

    public boolean isOrdinaryClass() {
        return _getExecutableMemberDoc().isOrdinaryClass();
    }

    public String name() {
        return _getExecutableMemberDoc().name();
    }

    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getExecutableMemberDoc().seeTags());
    }

    public void setRawCommentText(String szText) {
        _getExecutableMemberDoc().setRawCommentText(szText);
    }

    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getExecutableMemberDoc().tags());
    }

    public Tag[] tags(String szTagName) {
        return (Tag[])wrapOrHide(_getExecutableMemberDoc().tags(szTagName));
    }
   
    public SourcePosition position() {
        return (SourcePosition)
                wrapOrHide(_getExecutableMemberDoc().position());
    }

    public boolean isAnnotationType() {
        return _getExecutableMemberDoc().isAnnotationType();
    }

    public boolean isSynthetic() {
        return _getExecutableMemberDoc().isSynthetic();
    }

    public AnnotationDesc[] annotations() {
        return (AnnotationDesc[])
                wrapOrHide(_getExecutableMemberDoc().annotations());
    }

    public boolean isEnum() {
        return _getExecutableMemberDoc().isEnum();
    }

    public boolean isAnnotationTypeElement() {
        return _getExecutableMemberDoc().isAnnotationTypeElement();
    }

    public boolean isEnumConstant() {
        return _getExecutableMemberDoc().isEnumConstant();
    }
    
    public boolean isFinal() {
        return _getExecutableMemberDoc().isFinal();
    }

    public ClassDoc containingClass() {
        return (ClassDoc)
                wrapOrHide(_getExecutableMemberDoc().containingClass());
    }

    public PackageDoc containingPackage() {
        return (PackageDoc)
                wrapOrHide(_getExecutableMemberDoc().containingPackage());
    }

    public boolean isPackagePrivate() {
        return _getExecutableMemberDoc().isPackagePrivate();
    }

    public boolean isPrivate() {
        return _getExecutableMemberDoc().isPrivate();
    }

    public boolean isProtected() {
        return _getExecutableMemberDoc().isProtected();
    }

    public boolean isPublic() {
        return _getExecutableMemberDoc().isPublic();
    }

    public boolean isStatic() {
        return _getExecutableMemberDoc().isStatic();
    }

    public int modifierSpecifier() {
        return _getExecutableMemberDoc().modifierSpecifier();
    }

    public String modifiers() {
        return _getExecutableMemberDoc().modifiers();
    }

    public String qualifiedName() {
        return _getExecutableMemberDoc().qualifiedName();
    }
}
