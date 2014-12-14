/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SerialFieldTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;
import com.sun.javadoc.Type;

class HidingFieldDocWrapper extends HidingWrapper implements FieldDoc {

    public HidingFieldDocWrapper(FieldDoc fielddoc, Map mapWrappers) {
        super(fielddoc, mapWrappers);
    }

    private FieldDoc _getFieldDoc() {
        return (FieldDoc)getWrappedObject();
    }

    public boolean isTransient() {
        return _getFieldDoc().isTransient();
    }

    public boolean isVolatile() {
        return _getFieldDoc().isVolatile();
    }

    public SerialFieldTag[] serialFieldTags() {
        return (SerialFieldTag[])wrapOrHide(_getFieldDoc().serialFieldTags());
    }

    public Type type() {
        return (Type)wrapOrHide(_getFieldDoc().type());
    }

    public Object constantValue() {
        return _getFieldDoc().constantValue();
    }

    public String constantValueExpression() {
        return _getFieldDoc().constantValueExpression();
    }

    public AnnotationDesc[] annotations() {
        return (AnnotationDesc[])wrapOrHide(_getFieldDoc().annotations());
    }

    public boolean isEnum() {
        return _getFieldDoc().isEnum();
    }

    public boolean isAnnotationTypeElement() {
        return _getFieldDoc().isAnnotationTypeElement();
    }

    public boolean isEnumConstant() {
        return _getFieldDoc().isEnumConstant();
    }
    
    public boolean isSynthetic() {
        return _getFieldDoc().isSynthetic();
    }
  
    public String commentText() {
        return _getFieldDoc().commentText();
    }

    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getFieldDoc().
                   compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {  
            return _getFieldDoc().compareTo(obj);
        }
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getFieldDoc().firstSentenceTags());
    }

    public String getRawCommentText() {
        return _getFieldDoc().getRawCommentText();
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getFieldDoc().inlineTags());
    }

    public boolean isClass() {
        return _getFieldDoc().isClass();
    }

    public boolean isConstructor() {
        return _getFieldDoc().isConstructor();
    }

    public boolean isError() {
        return _getFieldDoc().isError();
    }

    public boolean isException() {
        return _getFieldDoc().isException();
    }

    public boolean isField() {
        return _getFieldDoc().isField();
    }

    public boolean isIncluded() {
        return _getFieldDoc().isIncluded();
    }

    public boolean isInterface() {
        return _getFieldDoc().isInterface();
    }

    public boolean isMethod() {
        return _getFieldDoc().isMethod();
    }

    public boolean isOrdinaryClass() {
        return _getFieldDoc().isOrdinaryClass();
    }

    public String name() {
        return _getFieldDoc().name();
    }

    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getFieldDoc().seeTags());
    }

    public void setRawCommentText(String szText) {
        _getFieldDoc().setRawCommentText(szText);
    }

    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getFieldDoc().tags());
    }

    public Tag[] tags(String szTagName) {
        return (Tag[])wrapOrHide(_getFieldDoc().tags(szTagName));
    }
   
    public SourcePosition position() {
        return (SourcePosition)wrapOrHide(_getFieldDoc().position());
    }

    public boolean isAnnotationType() {
        return _getFieldDoc().isAnnotationType();
    }
    
    public ClassDoc containingClass() {
        return (ClassDoc)wrapOrHide(_getFieldDoc().containingClass());
    }

    public PackageDoc containingPackage() {
        return (PackageDoc)wrapOrHide(_getFieldDoc().containingPackage());
    }

    public boolean isFinal() {
        return _getFieldDoc().isFinal();
    }
  
    public boolean isPackagePrivate() {
        return _getFieldDoc().isPackagePrivate();
    }

    public boolean isPrivate() {
        return _getFieldDoc().isPrivate();
    }

    public boolean isProtected() {
        return _getFieldDoc().isProtected();
    }

    public boolean isPublic() {
        return _getFieldDoc().isPublic();
    }

    public boolean isStatic() {
        return _getFieldDoc().isStatic();
    }

    public int modifierSpecifier() {
        return _getFieldDoc().modifierSpecifier();
    }

    public String modifiers() {
        return _getFieldDoc().modifiers();
    }

    public String qualifiedName() {
        return _getFieldDoc().qualifiedName();
    }
}
